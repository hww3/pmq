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
  call_out(run, 1);
  return;  
}

void run()
{
  reader = client->get_queue_reader("wunderbar");
}
