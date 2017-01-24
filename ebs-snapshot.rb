#!/usr/bin/env ruby

#
# EBS snapshoting tool. Can lock databases, freeze the filesystem and delete old snapshots.
#

require "rubygems"
require "chronic"
require "fog"
require "net/http"
require "socket"
require "slop"
require "time"
require "yaml"

class Hash
  def deep_merge(second)
    merger = proc{|key,v1,v2| Hash === v1 && Hash === v2 ? v1.merge(v2, &merger) : v2}
    self.merge(second, &merger)
  end
end

class Array
  # Snapshots are usually in sorted arrays, these helpers make it easier to read
  alias :oldest :first
  alias :newest :last
end

# Create and list snapshots for a (set of) ebs volume(s)
class Snapshot
  attr_accessor :type

  def initialize(options={})
    @fog         = options[:fog]
    @types       = options[:types]
    @volume_ids  = options[:volume_ids]
    @mount       = options[:mount]
    @hostname    = options[:hostname]
    @verbose     = options[:verbose]

    @all = nil
    @by_type = nil
  end

  # Array of { type_name => snapshot_array }
  def expired
    self.scan unless @expired
    @expired
  end

  # Array of type names that are needed
  def needed_types
    self.scan unless @needed_types
    @needed_types
  end

  # Scan all snapshots for expired and needed types
  def scan
    @needed_types = []
    @expired = []

    @types.each do |type, config|
      have = self.by_type[type]

      if have.nil?
        @needed_types << type
        next
      end

      expired_count = have.count - config["Keep"]
      if expired_count > 0
        have.oldest(expired_count).each do |set|
          @expired << {type => set}
        end
      end

      if have.newest[:created_at] <= Chronic.parse(config["Every"] + " ago")
        @needed_types << type
      end
    end
  end

  # Create the snapshots for needed_types
  def create!
    set_id = generate_set_id

    @volume_ids.each do |id|
      snapshot = @fog.snapshots.new

      snapshot.description = "#{@hostname.split(".")[0]} #{@mount} (#{self.needed_types.join(", ")}) (#{set_id})"
      snapshot.volume_id   = id

      # Actually do the snapshot
      snapshot.save

      # Reload to get snapshot.id so we can add tags
      snapshot.reload

      @fog.tags.create(:resource_id => snapshot.id, :key => "Host",  :value => @hostname)
      @fog.tags.create(:resource_id => snapshot.id, :key => "Mount", :value => @mount)
      @fog.tags.create(:resource_id => snapshot.id, :key => "SetID", :value => set_id)
      @fog.tags.create(:resource_id => snapshot.id, :key => "Type",  :value => self.needed_types.join(","))
    end
  end

  def generate_set_id
    rand(36**10).to_s(36)
  end

  # Sorted array of all snapshot sets
  def all
    return @all if @all

    @all = []
    sets = {}

    @fog.tags.all(:key => "Host", :value => @hostname).each do |tag|
      next unless tag.resource_type == "snapshot"
      next unless snap = @fog.snapshots.get(tag.resource_id)
      next unless snap.tags["Mount"] == @mount

      set_id = snap.tags["SetID"] || generate_set_id

      set = sets[set_id] ? sets[set_id] : {:ids => [], :created_at => snap.created_at, :types => []}

      set[:ids]        << snap.id
      set[:created_at]  = snap.created_at if snap.created_at < set[:created_at]
      set[:types]      |= snap.tags["Type"].split(",") if snap.tags.has_key?("Type")

      sets[set_id] = set
    end

    @all = sets.values
    @all.sort!{|a, b| a[:created_at] <=> b[:created_at]}

    @all
  end

  # Hash of sorted snapshot sets by type
  def by_type
    return @by_type if @by_type

    @by_type = {}
    self.all.each do |set|
      next unless set[:types].count > 0

      set[:types].each do |t|
        @by_type[t] ||= []
        @by_type[t] << set
      end
    end

    @by_type
  end

  # Remove Type tags and/or delete snapshots as needed
  def expire!
    self.expired.each do |x|
      expired_type, set = x.shift

      new_type = (set[:types].select{|i| i != expired_type})
      if new_type.count == 0
        puts "Deleting snapshot set created at #{set[:created_at]}" if @verbose
        set[:ids].each{|id| @fog.delete_snapshot(id)}
      else
        puts "Retagging snapshot set created at #{set[:created_at]}" if @verbose
        set[:ids].each{|id| @fog.tags.create(:resource_id => id, :key => "Type", :value => new_type.join(","))}
      end
    end
  end

  # Basic sanity check of exisiting snapshots
  def valid_since?(time)
    @types.each do |type, config|
      have = self.by_type[type].reject{|s| s[:created_at] < time} || []

      interval = (Time.now - Chronic.parse(config["Every"] + " ago")).to_i

      expected_count = (Time.now - time).to_i/interval.to_i
      expected_count = config["Keep"] if expected_count > config["Keep"]

      next if expected_count == 0

      expected_oldest = Time.now - (expected_count-1 * interval)
      expected_newest = Time.now - (interval * 2)

      count_ok = (have.count >= expected_count)
      oldest_ok = have.count > 0 && (have.oldest[:created_at] <= expected_oldest)
      newest_ok = have.count > 0 && (have.newest[:created_at] >= expected_newest)

      if @verbose
        have_oldest = have.oldest ? have.oldest[:created_at] : "N/A"
        have_newest = have.newest ? have.newest[:created_at] : "N/A"
        puts "Type: #{type}"
        puts "Have: #{have.count} Expected: #{expected_count}: (#{count_ok})"
        puts "Oldest: #{have_oldest} Expected: #{expected_oldest} or older: (#{oldest_ok})"
        puts "Newest: #{have_newest} Expected: #{expected_newest} or newer: (#{newest_ok})"
      end

      return false unless count_ok && oldest_ok && newest_ok
    end

    return true
  rescue Fog::Compute::AWS::Error
    return nil
  end
end

# Determine devices, EBS volume-id(s) and filesystem from a mountpoint
class Mount
  attr_accessor :devices
  attr_accessor :filesystem

  def initialize(options={})
    fog          = options[:fog]
    instance_id  = options[:instance_id]
    wanted_mount = options[:mount]

    # Try to find volume-id(s) from a mount
    @devices = {}
    @filesystem = nil
    @mount = options[:mount]

    found_devices = nil

    File.open("/etc/mtab", "r").each_line do |line|
      # /dev/sdg /backup xfs rw 0 0
      dev, mount, fs = line.split(" ")
      next unless mount == wanted_mount

      @filesystem = fs
      dev = dev.split("/").last
      found_devices = dev.match(/^md\d+$/) ? dev_from_md(dev) : [ dev ]
      break
    end

    raise "Couldn't find #{wanted_mount} in mtab" unless found_devices

    # Newer kernels identify as xvd<X> but Amazon says still says sd<X>
    found_devices = found_devices.map{|d| d.gsub(/^xvd/, "sd")}

    # Strip off partition number
    found_devices = found_devices.map{|d| d.gsub(/[0-9]+$/, "")}

    fog.servers(:filters => {"instance-id" => instance_id}).first.volumes.each do |vol|   
      dev = vol.device.split("/").last.gsub(/[0-9]+$/, "")
      next unless found_devices.include? dev

      @devices[dev] = vol.id
    end

    raise "Could not find volume-ids devices" if @devices.count != found_devices.count
  end

  def volume_ids
    @devices.values
  end

  def dev_from_md(wanted_md)
    devices = nil

    File.open("/proc/mdstat", "r").each_line do |line|
      # md0 : active raid0 sdd[1] sdc[0] sdf[3] sde[2]
      md, devices = line.scan(/^(md\d+)\s*:\s*active raid\d+\s*(.*)$/).first
      next unless md == wanted_md

      devices = devices.gsub!(/\[\d+\]/, "").split(" ")
      break
    end

    devices
  end

  def lock!
    system "sudo sync"

    case @filesystem
    when "xfs"
      system "sudo xfs_freeze -f #{@mount}"
    end
  end

  def unlock!
    case @filesystem
    when "xfs"
      system "sudo xfs_freeze -u #{@mount}"
    end
  end
end

class Databases
  attr_accessor :connections

  def initialize(uris = [])
    @connections = []

    Array(uris).each do |u|
      # Try to fake a URI if just a name was specified.
      u += "://localhost" unless (u =~ URI::regexp)

      (scheme, userinfo, host, port) = URI.split(u)
      (user, password) = userinfo.split(":") if userinfo

      case scheme
      when "mongodb"
        require "mongo"
        begin
          @connections.push({:mongo => Mongo::Connection.new(host, port)})
        rescue Mongo::ConnectionFailure
          $stderr.puts "Failed to connect to database, skipping."
        end
      when "mysql"
        require "mysql"
        begin
          @connections.push({:mysql => Mysql.new(host, user, password, nil, port)})
        rescue Mysql::Error
          $stderr.puts "Failed to connect to database, skipping."
        end
      when "redis"
        require "redis"
        begin
          conn = Redis.new(:host => host, :port => port, :password => password)
          conn.ping
          @connections.push({:redis => conn})
        rescue SocketError
          $stderr.puts "Failed to connect to database, skipping."
        end
      end
    end
  end

  def lock!
    @connections.each do |c|
      (type, conn) = c.to_a.first
      case type
      when :mysql
        conn.query "FLUSH LOCAL TABLES"
        conn.query "FLUSH TABLES WITH READ LOCK"
      when :mongo
        conn.lock!
      when :redis
        conn.save
      end
    end
  end

  def unlock!
    @connections.each do |c|
      (type, conn) = c.to_a.first
      case type
      when :mysql
        conn.query "UNLOCK TABLES"
      when :mongo
        conn.unlock!
      end
    end
  end
end

options = Slop.parse do |o|
  o.bool    "--all",    "Preform all volume and clean tasks."
  o.string "--check",   "Check that minimum snapshots exist since given time."
  o.string "--config",  "Configuration directory to use", :default => "/etc/ebs-snapshot"
  o.bool   "--dry-run", "Only print actions that would be taken."
  o.bool   "--verbose", "Print verbose messages."
  o.bool   "--force",   "Force snapshot."
  o.array  "--volume",  "Volume name from configuration."
  o.on "--help" do
    puts o
    exit
  end
end

raise "No such directory" unless File.directory?(options[:config])

config = {}
Dir.glob(options[:config] + "/*.yml").each do |f|
  config = config.deep_merge YAML.load(File.open(f))
end

raise "Invalid configuration." unless config["AWS"] && config["Types"] && config["Volumes"]

fog = Fog::Compute.new(
  :provider => "AWS",
  :aws_access_key_id => config["AWS"]["Key"],
  :aws_secret_access_key => config["AWS"]["Secret"]
)

raise "You must specify a list of volumes, or --all" unless options[:volume] || options[:all]

config[:hostname] ||= Socket.gethostname
volumes = options.all? ? config["Volumes"].keys : Array(options[:volume])

config[:instance_id] ||= Net::HTTP.get(URI("http://169.254.169.254/latest/meta-data/instance-id"))
raise "Couldn't determin instance-id" unless config[:instance_id]

check = {:messages => [], :rc => 0}

volumes.each do |vol_name|
  puts "Checking Volume: #{vol_name}" if options[:verbose]
  vol_config = config["Volumes"][vol_name]

  mount = Mount.new(
    :fog          => fog,
    :instance_id  => config[:instance_id],
    :mount        => vol_config["Mount"]
  )

  if mount.volume_ids.nil?
    $stderr.puts "Unable to determine volume id from mount #{vol_config["Mount"]}"
    next
  end

  snapshot = Snapshot.new(
    :fog        => fog,
    :hostname   => config[:hostname],
    :mount      => vol_config["Mount"],
    :types      => config["Types"],
    :volume_ids => mount.volume_ids,
    :verbose    => options.verbose?
  )

  if options[:check]
    case snapshot.valid_since?(Chronic.parse(options[:check] + " ago"))
    when true
      check[:messages] << "#{snapshot.all.count} snapshots for #{vol_name}"
    when nil
      check[:messages] << "Failed to access the AWS API"
      check[:rc] = 3
    else
      check[:messages] << "missing snapshots for #{vol_name}"
      check[:rc] = 2
    end
    next
  end

  if options[:verbose]
    puts "- Total snapshots: #{snapshot.all.count}"
    puts "- Newest: #{snapshot.all.newest[:created_at]}" if snapshot.all.count > 0
    puts "- Oldest: #{snapshot.all.oldest[:created_at]}" if snapshot.all.count > 0
  end

  snapshot.needed_types << "forced" if options[:force]

  if snapshot.needed_types.count > 0
    puts "- Snapshot needed for #{snapshot.needed_types.join(", ")} type(s)." if options[:verbose]

    dbs = Databases.new(vol_config["Database"])

    begin
      puts "- Locking databases" if options[:verbose]
      dbs.lock! unless options[:dry_run]

      puts "- Locking filesystem" if options[:verbose]
      mount.lock! unless options[:dry_run]

      puts "- Taking snapshot" if options[:verbose]
      snapshot.create! unless options[:dry_run]

    rescue Exception => e
      $stderr.puts e.message
      $stderr.puts "! Aborted, attempting to unlock."
      exit 1
    ensure
      puts "- Unlocking filesystem" if options[:verbose]
      mount.unlock! unless options[:dry_run]

      puts "- Unlocking databases" if options[:verbose]
      dbs.unlock! unless options[:dry_run]
    end
  else
    puts "- No snapshots needed" if options[:verbose]
  end

  if snapshot.expired.count > 0
    puts "- Expiring #{snapshot.expired.count} snapshot(s)" if options[:verbose]
    snapshot.expire! unless options[:dry_run]

  else
    puts "- No snapshots to expire" if options[:verbose]
  end
end

if options[:check]
  check_word = begin
    case check[:rc]
    when 0
      "OK"
    when 1
      "WARNING"
    when 2
      "CRITICAL"
    when 3
      "UNKNOWN"
    end
  end

  puts "#{check_word}: #{check[:messages].join(";")}"
  exit check[:rc]
end
