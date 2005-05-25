import PMQ;
PMQQueueReader reader;
PMQProperties config;
PMQClient client;
import PMQConstants;

int main(int argc, array argv)
{
  DEBUG_LEVEL(1);  
  call_out(create_connection, 1);
  return -1; 
}

void create_connection()
{
  write(sprintf("Connecting to pmqd... "));
  client = PMQClient("pmq://127.0.0.1:9999");
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
  string s = "<?xml version=\"1.0\" ?><t><e>hi</e></t>";
int i = 0;
  do
  {
//    Stdio.stdin.gets();
    object m = Message.PMQMessage();
    m->set_body(s);
    reader->post(m);
  write("wrote message.\n");
i++;
  } while(i<1000);

  exit(0);
}
