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
//    string qn = id->misc->path_info;
//    if(qn[0..0] == "/") qn = qn[1..];
   return 1;
   }


  else return 1;
}

int listen(object id, string destination)
{
    READER = CLIENT->get_queue_reader(destination);
}

mixed parse(object id)
{
   PMQ.Message.PMQMessage m;

werror("*** parse: %O\n\n", id->variables); 
   int timeout = id->variables->timeout;
   if(timeout) timeout/=1000;
   else timeout=30;

   if(id->variables->shutdown)
   {
     READER = 0;
     CLIENT->disconnect();
     CLIENT = 0;
     return "ok";
   }
   if(id->variables->message) id->misc->session_variables->message = id->variables->message;

   if(!CLIENT && !connect(id))
   {
     CLIENT = 0;
     return "PMQD not available.";
   }

   if(id->variables->type == "listen")     
   {                                       
     listen(id, id->variables->destination);
   }  
  
  if(READER && catch(m = READER->read(timeout)))
  {
    CLIENT = 0;
    READER = 0;
//    return "caught an error!";
  }
  else 
		    return Caudium.HTTP.string_answer("<?xml version=\"1.0\"?><ajax-response><messages id='" + (id->misc->session_variables->message||"id") + "'><message>" + (m?m->get_body():"") + "</message></messages></ajax-response>", "text/xml");
}

void stop()
{
}
