import PMQ;
import PMQConstants;
PMQReader reader;
PMQProperties config;
PMQClient client;

int main(int argc, array argv)
{
  DEBUG_LEVEL(0);
  call_out(create_connection, 0);
  return -1; 
}

void create_connection()
{
  write(sprintf("Connecting to pmqd... "));
  client = PMQClient("pmq:///tmp/pmqd.sock");
//  client = PMQClient("pmq://127.0.0.1:9999");
  client->connect();
  call_out(run, 0);
  return;  
}

void run()
{

  reader = client->get_queue_reader("wunderbar");
int i = 0;
//  reader->set_read_callback(lambda(){i++; if(i%100 == 0) write("got " + 
//i + " messages.\n"); });
//  reader->start();
//return;
  write("starting reader...\n");
  do
  {
    Message.PMQMessage m = reader->read();
//    werror("reader got a message: %O\n", m);
i++;
if(i%100 == 0) write("got " + i + " messages.\n");
if(i == 200) { destruct(reader); break;}
  }
  while(1);
  call_out(run, 0);
}

