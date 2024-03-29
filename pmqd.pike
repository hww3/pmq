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

  string c = Stdio.read_file("pmqd.config");

  if(c)  config->load(c);

  return;
}

void setup_port()
{

  if(config->get("pmqd.domainsocket.listenpath"))
  {
    werror("starting pmqd with a listener at " + 
      config->get("pmqd.domainsocket.listenpath") + ".\n");
    if(Stdio.exist(config->get("pmqd.domainsocket.listenpath")))
      rm(config->get("pmqd.domainsocket.listenpath"));

    port->bind_unix("/tmp/pmqd.sock",
//config->get("pmqd.domainsocket.listenpath"), 
      accept_connection);
  }
  else if(config->get("pmqd.tcpsocket.listenport"))
  {
    int port = (int)config->get("pmqd.tcpsocket.listenport");
    if(!port) port = 9999;
  
    werror("starting pmqd with a listener on " + port + ".\n");
    this->port->bind(port, accept_connection);  
  }

  else
  {
    werror("no port or domain socket path specified!\n");
    exit(1);
  }
}

void accept_connection(mixed id)
{
  PMQSConnection conn;
  conn = PMQSConnection(port->accept(), config, connections, packets);
  conn->set_queue_manager(manager);  
  conn->go();
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
