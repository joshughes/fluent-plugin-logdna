require 'fluent/output'
require 'oj'

module Fluent
  class LogDNAOutput < Fluent::BufferedOutput
    Fluent::Plugin.register_output('logdna', self)

    INGESTER_DOMAIN = 'https://logs.logdna.com'.freeze
    MAX_RETRIES = 5

    config_param :api_key, :string, secret: true
    config_param :hostname, :string
    config_param :hostname_record_key, :string, default: nil
    config_param :app_record_key, :string, default: nil
    config_param :mac, :string, default: nil
    config_param :ip, :string, default: nil
    config_param :app, :string, default: nil

    def configure(conf)
      super
      @host = conf['hostname']
    end

    def start
      super
      require 'json'
      require 'base64'
      require 'http'
      HTTP.default_options = { :keep_alive_timeout => 60 }
      @ingester = HTTP.persistent INGESTER_DOMAIN
      @requests = Queue.new
    end

    def shutdown
      super
      @ingester.close if @ingester
    end

    def format(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def write(chunk)
      body = chunk_to_body(chunk)
      response = send_request(body, host)
      raise 'Encountered server error' if response.code >= 400
      response.flush
    end

    private

    def chunk_to_body(chunk)
      data = []
      host = ''

      chunk.msgpack_each do |(tag, time, record)|
        line, host = gather_line_data(tag, time, record)
        data << line unless line[:line].empty?
      end

      { lines: data }, host
    end

    def gather_line_data(tag, time, record)
      line = {
        level: record['level'] || record['severity'] || tag.split('.').last,
        timestamp: time,
        line: record['message'] || record.to_json
      }
      
      if @app_record_key && record.has_key?(@app_record_key)
        line[:app] = record[@app_record_key]
      else
        line[:app] = record['_app'] || record['app']
        line[:app] ||= @app if @app
      end
      
      line.delete(:app) if line[:app].nil?
      
      if @hostname_record_key && record.has_key?(@hostname_record_key)
        host = record[@hostname_record_key]
      else
        host = @host
      end
      
      line, host
    end

    def send_request(body, host)
      now = Time.now.to_i
      url = "/logs/ingest?hostname=#{host}&mac=#{@mac}&ip=#{@ip}&now=#{now}"
      @ingester.headers('apikey' => @api_key,
                        'content-type' => 'application/json')
               .post(url, json: body)
    end
  end
end
