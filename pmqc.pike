PMQQueueReader reader;
PMQProperties config;
PMQClient client;
import PMQConstants;

int main(int argc, array argv)
{
  DEBUG_LEVEL(10);  
  call_out(create_connection, 1);
  return -1; 
}

void create_connection()
{
  write(sprintf("Connecting to pmqd... "));
  client = PMQClient("pmq://buoy.riverweb.com:9999");
  client->connect();
  call_out(run, 0);
  return;  
}

void run()
{
  reader = client->get_queue_writer("wunderbar");
  call_out(post, 0);
}

void post()
{
 gauge(do_post());
}
void do_post()
{
//  Public.Parser.XML2.Node n;

  string s = "<?xml version=\"1.0\" ?><t><e>hi</e></t>";
    object m = Message.PMQMessage();
    m->set_body(s);
    reader->post(m);
  write("wrote message.\n");
call_out(do_post, 5);
  exit(0);
}
