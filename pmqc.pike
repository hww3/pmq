import PMQ;
PMQReader reader;
PMQProperties config;
PMQClient client;
import PMQConstants;

int main(int argc, array argv)
{
  DEBUG_LEVEL(0);  
  call_out(create_connection, 1);
  return -1; 
}

void create_connection()
{
  write(sprintf("Connecting to pmqd... "));
//  client = PMQClient("pmq://127.0.0.1:9999");
  client = PMQClient("pmq:///tmp/pmqd.sock");
  client->connect();
  call_out(run, 0);
  return;  
}

void run()
{
  call_out(post, 0);
}

void post()
{
 gauge(do_post());
}
void do_post()
{  string s = "<?xml version=\"1.0\" ?><t><e>hi</e></t>";
int i = 0;
    reader = client->get_queue_writer("wunderbar");
do{
for (int k = 0; k < 3000; k++)
{
    object m = Message.PMQMessage();
    m->set_body(s);
    reader->write(m);
}
//  sleep(random(5));
} while(0);
  write("wrote message.\n");
  exit(0);
}
	
