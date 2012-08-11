# gem install amqp

# require 'rubygems'
require 'amqp'
require 'java'

Dir.glob("lib/*.jar").each {|lib| require lib}

java_import 'com.espertech.esper.client.EPRuntime'
java_import 'com.espertech.esper.client.EPServiceProviderManager'
java_import 'com.espertech.esper.client.EPServiceProvider'
java_import 'com.espertech.esper.client.Configuration'
java_import 'com.espertech.esper.client.ConfigurationEventTypeXMLDOM'
java_import 'com.espertech.esper.client.EPStatement'

java_import 'com.espertech.esper.client.UpdateListener'
java_import 'com.espertech.esper.client.EventBean'
java_import 'org.apache.commons.logging.Log'
java_import 'org.apache.commons.logging.LogFactory'

java_import 'javax.xml.parsers.DocumentBuilder'
java_import 'javax.xml.parsers.DocumentBuilderFactory'
java_import 'java.io.ByteArrayInputStream'
java_import 'java.io.StringReader';

include Java

# Listener class for esper updates
class EsperListener
  include UpdateListener

  def initialize(exchange, queue)
    @exchange, @queue = exchange, queue
    puts "Initialized EsperListener"
  end

  def update(newEvents, oldEvents)
    newEvents.each do |event|
      puts "New event: #{event.getUnderlying.toString}"
      @exchange.publish "Esper processed event: #{event.getUnderlying.toString}", :routing_key => @queue.name
      puts "Pushed processed event to #{@queue.name}"
    end
  end
end

EventMachine.run do

  # Setup rabbit-mq
  connection = AMQP.connect(:host => '127.0.0.1')
  puts "Connecting to AMQP broker. Running #{AMQP::VERSION} version of the gem..."

  channel  = AMQP::Channel.new(connection)
  read_queue    = channel.queue("my_read_queue", :auto_delete => true)
  write_queue   = channel.queue("my_write_queue", :auto_delete => true)
  exchange = channel.default_exchange

  # Setup Esper via API.
  # Possible to use xml config file
  config = Configuration.new
  # config.configure("esper.cfg.xml")
  desc = ConfigurationEventTypeXMLDOM.new
  desc.setRootElementName("Event")
  desc.addNamespacePrefix("event", "http://cep.true-synergy.nl")
  desc.setRootElementNamespace("event")
  desc.setDefaultNamespace("event")

  config.addEventType("MyXMLNodeEvent", desc)

  epService = EPServiceProviderManager.getDefaultProvider(config)

  listener = EsperListener.new(exchange, write_queue)
  statement = epService.getEPAdministrator.createEPL("select email,eventID from MyXMLNodeEvent")
  statement.addListener(listener)

  # Second statement
  epService.getEPAdministrator.createEPL("select count(*) from MyXMLNodeEvent.win:time(3 min) output snapshot every 5 seconds").addListener(EsperListener.new(exchange, write_queue))

  read_queue.subscribe do |payload|
    puts "Received a message. Pushing to Esper"
    # EventMachine.defer do
       factory = DocumentBuilderFactory.newInstance()
       factory.setNamespaceAware(true);
       node = factory
         .newDocumentBuilder()
         .parse(ByteArrayInputStream.new(payload.to_java_bytes))
         .getDocumentElement();
    epService.getEPRuntime.sendEvent(node)
    # end

  end

  # hit Control + C to stop
  Signal.trap("INT")  { connection.close { EventMachine.stop }}
  Signal.trap("TERM") { connection.close {EventMachine.stop }}

end
