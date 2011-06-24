//PMQ.PMQQueueReader reader = 0;
PMQ.PMQProperties config;
//PMQ.PMQClient client = 0;
import PMQ.PMQConstants;

#define CLIENT id->misc->session_variables->client
#define READER id->misc->session_variables->reader

void create()
{
  write("created queue reader\n");
  gc();
}

void destroy()
{
}

int connect(object id)
{
werror("connecting?\n");
  if(!CLIENT)
  {
    CLIENT = PMQ.PMQClient("pmq://127.0.0.1:9999");
    if(!CLIENT->connect()) return 0;
werror("connected.\n");
    string qn = id->misc->path_info;
    if(qn[0..0] == "/") qn = qn[1..];
    READER = CLIENT->get_queue_reader(qn);
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
     READER = 0;
     CLIENT->disconnect();
     CLIENT = 0;
     return "ok";
   }

   if(!connect(id))
   {
     CLIENT = 0;
     return "PMQD not available.";
   }
 
  if(catch(m = READER->read()))
  {
    CLIENT = 0;
    READER = 0;
//    return "caught an error!";
  }
  else 
    return (string)m;
}

void stop()
{
}
