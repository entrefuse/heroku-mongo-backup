# encoding: UTF-8

require 'mongo'
require 'json'
require 'zlib'
require 'uri'
require 'yaml'
require 'rubygems'
require 'net/ftp'

module HerokuMongoBackup

  class << self
    attr_accessor :aws_bucket, :aws_access_key_id, :aws_secret_access_key,
                  :mongo_username, :mongo_password, :mongo_host, :mongo_database, :mongo_url
  end

  if defined?(Rails::Railtie)
    class Railtie < Rails::Railtie
      rake_tasks do
        load "tasks/heroku_mongo_backup.rake"
      end
    end
  end

  require 's3_helpers'

  class Backup
    def chdir
      Dir.chdir("/tmp")
      begin
        Dir.mkdir("dump")
      rescue
      end
      Dir.chdir("dump")
    end

    def store
      backup = {}
  
      @db.collections.each do |col|
        backup['system.indexes.db.name'] = col.db.name if col.name == "system.indexes"
    
        records = []
    
        col.find().each do |record|
          records << record
        end

        backup[col.name] = records
      end
  
      marshal_dump = Marshal.dump(backup)
  
      file = File.new(@file_name, 'w')
      file.binmode
      file = Zlib::GzipWriter.new(file)
      file.write marshal_dump
      file.close
    end

    def load
      file = Zlib::GzipReader.open(@file_name)
      obj = Marshal.load file.read
      file.close

      obj.each do |col_name, records|
        next if col_name =~ /^system\./
    
        @db.drop_collection(col_name)
        dest_col = @db.create_collection(col_name)
    
        records.each do |record|
          dest_col.insert record
        end
      end
  
      # Load indexes here
      col_name = "system.indexes"
      dest_index_col = @db.collection(col_name)
      obj[col_name].each do |index|
        if index['_id']
          index['ns'] = index['ns'].sub(obj['system.indexes.db.name'], dest_index_col.db.name)
          dest_index_col.insert index
        end
      end
    end

    def db_connect
      uri = URI.parse(@url)
      connection = ::Mongo::Connection.new(uri.host, uri.port)
      @db = connection.db(uri.path.gsub(/^\//, ''))
      @db.authenticate(uri.user, uri.password) if uri.user
    end
    
    def ftp_connect
      @ftp = Net::FTP.new(ENV['FTP_HOST'])
      @ftp.passive = true
      @ftp.login(ENV['FTP_USERNAME'], ENV['FTP_PASSWORD'])
    end
    
    def ftp_upload
      @ftp.putbinaryfile(@file_name)
    end
    
    def ftp_download
      open(@file_name, 'w') do |file|
        file_content = @ftp.getbinaryfile(@file_name)
        file.binmode
        file.write file_content
      end
    end
    
    def s3_connect
      @bucket = HerokuMongoBackup::s3_connect(HerokuMongoBackup.aws_bucket, HerokuMongoBackup.aws_access_key_id, HerokuMongoBackup.aws_secret_access_key)
    end

    def s3_upload
      HerokuMongoBackup::s3_upload(@bucket, @file_name)
    end

    def s3_download
      open(@file_name, 'w') do |file|
        file_content = HerokuMongoBackup::s3_download(@bucket, @file_name)
        file.binmode
        file.write file_content
      end
    end

    def initialize connect = true
      @file_name = Time.now.strftime("%Y-%m-%d_%H-%M-%S.gz")

      if HerokuMongoBackup.mongo_url
        @url = HerokuMongoBackup.mongo_url
      elsif HerokuMongoBackup.mongo_username
        @url = "mongodb://#{HerokuMongoBackup.mongo_username}:#{HerokuMongoBackup.mongo_password}@#{HerokuMongoBackup.mongo_host}/#{HerokuMongoBackup.mongo_database}"
      else
        @url = "mongodb://#{HerokuMongoBackup.mongo_host}/#{HerokuMongoBackup.mongo_database}"
      end
  
      puts "Using database: #{@url}"
  
      self.db_connect

      if connect
        if ENV['UPLOAD_TYPE'] == 'ftp'
          self.ftp_connect
        else
          self.s3_connect
        end
      end
    end

    def backup files_number_to_leave=0
      self.chdir    
      self.store

      if ENV['UPLOAD_TYPE'] == 'ftp'
        self.ftp_upload
        @ftp.close
      else
        self.s3_upload
      end

      if files_number_to_leave > 0
        HerokuMongoBackup::remove_old_backup_files(@bucket, files_number_to_leave)
      end
    end
    
    def restore file_name, download_file = true
      @file_name = file_name
  
      self.chdir
      
      if download_file
        if ENV['UPLOAD_TYPE'] == 'ftp'
          self.ftp_download
          @ftp.close
        else
          self.s3_download
        end
      end

      self.load
    end
  end
end
