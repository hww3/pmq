PMQ.PMQQueueReader reader = 0;
PMQ.PMQProperties config;
PMQ.PMQClient client = 0;
import PMQ.PMQConstants;

void create()
{
  write("created queue reader\n");
  gc();
}

void destroy()
{
  write("destroying queue reader\n");
  destruct(reader);
  destruct(client);
}

int connect(object id)
{
werror("connecting?\n");
  if(! client)
  {
    client = PMQ.PMQClient("pmq://127.0.0.1:9999");
    if(!client->connect()) return 0;
werror("connected.\n");
    string qn = id->misc->path_info;
    if(qn[0..0] == "/") qn = qn[1..];
    reader = client->get_queue_reader(qn);
   return 1;
   }


  else return 1;
}

mixed parse(object id)
{
   PMQ.Message.PMQMessage m;

werror("*** parse!\n"); 

   if(id->variables->shutdown)
   {
     reader = 0;
     client->disconnect();
     client = 0;
     return "ok";
   }

   if(!connect(id))
   {
     client = 0;
     return "PMQD not available.";
   }
 
  if(catch(m = reader->read()))
  {
    client = 0;
    reader = 0;
    return "caught an error!";
  }
  else 
    return (string)m;
}

void stop()
{
  if(client)
  {
    client->disconnect();
    destruct(client);
  } 

  if(reader)
    destruct(reader);
}
