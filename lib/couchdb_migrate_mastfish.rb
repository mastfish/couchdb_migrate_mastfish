class Couchdb_migrate_mastfish

  def initialize(config)
    @db = config["db"]
    @views = config["views"]
    @views.each do |key, value|
      # Define method
      # get_#{view["key"]}s}
      #   urls = db.view(value)
      #   inject urls into internal map method
      # end

      ######################
      # transform_#{view["key"]}s}
      #   urls = db.view(value)
      #   inject urls into internal each, update method
      #   accept block to do transformation here
      # end

    end
  end

  def update(http, iter)
    item = JSON.parse http.response
    item = transform(item) # execute block here
    put = EventMachine::HttpRequest.new(@db + item["_id"]).aput :body => item.to_json
    put.callback do
      iter.next
    end
  end

  def update_plays
    EM.synchrony do
      concurrency = 50
      urls = db.view('plays/all')

      # iterator will execute async blocks until completion, .each, .inject also work!
      results = EM::Synchrony::Iterator.new(urls["rows"], concurrency).each do |url, iter|
        # fire async requests, on completion advance the iterator
        http = EventMachine::HttpRequest.new(@db + url["id"]).aget
        http.callback { update(http, iter) }
      end
      EventMachine.stop
    end
  end
end
