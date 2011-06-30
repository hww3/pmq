import PMQ;
import PMQConstants;
//PMQReader reader;
PMQProperties config;
PMQClient client;// = PMQClient("pmq://127.0.0.1:9999");

int main(int argc, array argv)
{
  DEBUG_LEVEL(5);
  call_out(create_connection, 0);
  return -1; 
}

void create_connection()
{
  write(sprintf("Connecting to pmqd... "));
  client = PMQClient("pmq://127.0.0.1:9998");
  write("connected: %O\n", client->connect());
  call_out(run, 0);
  return;  
}
 PMQReader reader = 0;
int x;
void run()
{
  if(!reader){
reader =  client->get_topic_reader("observations", MODE_DELIVER_ACK);
reader->start();}
//gc();
  call_out(run,5);
  int i = 0;
//  write("starting reader...\n");
x++;
if(x > 3)
  do
  {
    Message.PMQMessage m = reader->read();
write("message %O: %O\n",++i, m);

  }
  while(i<5);
destruct(reader);
}

