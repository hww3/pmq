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
//  client = PMQClient("pmq:///tmp/pmqd.sock");
  client = PMQClient("pmq://127.0.0.1:9999");
  client->connect();
  call_out(run, 0);
  return;  
}

void run()
{
mixed g = gauge {
  reader = client->get_queue_reader("wunderbar", MODE_DELIVER_ACK);
int i = 0;
  write("starting reader...\n");
  do
  {
    Message.PMQMessage m = reader->read();
werror("got message: %O\n", m);
i++;
if(i%100 == 0) write("got " + i + " messages.\n");
  }
  while(i < 999);
};

werror("time: %O\n", g);
//  call_out(run, 0);
}

