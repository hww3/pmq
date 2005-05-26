Stdio.Port port = Stdio.Port();;
import PMQ;
PMQProperties config;
PMQQueueManager manager = PMQQueueManager();
import PMQConstants;

multiset connections = (<>);
mapping packets = ([]);

int main(int argc, array argv)
{
  DEBUG_LEVEL(0);
  read_config();
  setup_port();
  register_packet();
  call_out(report_connection, 1);
  signal(signum("INT"), quit);
  return -1; 
}

void quit()
{
  foreach((array)connections;;PMQConnection c)
    destruct(c);
  exit(0);
}

void report_connection()
{
  write(sprintf("Connections: %O\n", connections));
  call_out(report_connection, 10);
}

void read_config()
{
  config = PMQ.PMQProperties();

  config->set("pmqd.queue.autocreate", "1");
  config->set("pmqd.topic.autocreate", "1");
  return;
}

void setup_port()
{
  port->bind(9999, accept_connection);  
}

void accept_connection(mixed id)
{
  PMQSConnection conn;
  conn = PMQSConnection(port->accept(), config, connections, packets);
  conn->set_queue_manager(manager);  
  connections[conn] = 1;
}

void register_packet()
{
  packets=([]);

  foreach(values(Packet), program c)
  {
    object d = c();
    if(d->type)
    {
     write("startup: registering packet " + d->type + "\n");
     packets[d->type] = c;
    }
  }

}

