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
  client = PMQClient("pmq://127.0.0.1:9999");
  client->connect();
  call_out(run, 0);
  return;  
}

void run()
{

  Stdio.stdin.gets();
  reader = client->get_queue_reader("wunderbar");
int i = 0;
write("starting reader...\n");
  do
  {
    Message.PMQMessage m = reader->read();
//    werror("reader got a message: %O\n", m);
i++;
write("msgs: " + i + "\n");
  }
  while(1);
}

