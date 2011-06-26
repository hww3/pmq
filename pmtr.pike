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

  reader = client->get_topic_reader("observations", MODE_DELIVER_ACK);
int i = 0;
  write("starting reader...\n");
  do
  {
    Message.PMQMessage m = reader->read();
write("message %O: %O\n",++i, m);

  }
  while(i < 999);


//  call_out(run, 0);
}

