require 'amqp'

EventMachine.run do
  # Setup rabbit-mq
  connection = AMQP.connect(:host => '127.0.0.1')
  puts "Connecting to AMQP broker. Running #{AMQP::VERSION} version of the gem..."

  channel  = AMQP::Channel.new(connection)
  read_queue    = channel.queue("my_read_queue", :auto_delete => true)
  write_queue   = channel.queue("my_write_queue", :auto_delete => true)
  exchange = channel.default_exchange

  xml = <<XML
  <event:Event xmlns:event="http://x.com">
  <event:custID>123abc</event:custID>
  <event:status>cleared</event:status>
  <event:email></event:email>
  <event:host>gmdh-nt01</event:host>
  <event:timeDate>5-5-2012 6:85</event:timeDate>
  <event:description>Problemen met HTTP</event:description>
  <event:component>HTTP</event:component>
  <event:severity>Error</event:severity>
  <event:eventID>1234</event:eventID>
  </event:Event>
XML

  num_of_messages = ARGV[0].to_i || 50_000
  puts "Writing #{num_of_messages} messages to queue: #{read_queue.name}.."
  num_of_messages.times { exchange.publish xml, :routing_key => read_queue.name }

end
