import PMQ;
PMQReader reader;
PMQProperties config;
PMQClient client;
Pike.Backend backend;
import PMQConstants;

int main(int argc, array argv)
{
  DEBUG_LEVEL(0);
  backend = Pike.Backend();
  call_out(create_connection, 1);
  return -1; 
}

void create_connection()
{
mixed g = gauge{
for(int i = 0; i < 2; i++){
if(client) destruct(client);
  write(sprintf("Connecting to pmqd... "));
  client = PMQClient("pmq://127.0.0.1:9999");
//client->set_backend(backend);
  client->connect();
do_post();
}
};
werror("time: %O\n", g);
exit(0);
}

void run()
{
  call_out(post, 0);
}

void post()
{
 gauge(do_post());
}
int x = 0;

void do_post()
{
int i = 0;
    reader = client->get_queue_writer("wunderbar");
for (int k = 0; k < 5; k++)
{
                                             
  string s = "<t><e>hi+" + x + "</e></t>"; 
x++;   
    object m = Message.PMQMessage();
    m->set_body(s);
    reader->write(m);
}
  write("wrote message.\n");

  return;
}
	
