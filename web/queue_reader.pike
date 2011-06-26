//PMQ.PMQQueueReader reader = 0;
PMQ.PMQProperties config;
//PMQ.PMQClient client = 0;
import PMQ.PMQConstants;

#define CLIENT id->misc->session_variables->client
#define READER id->misc->session_variables->reader

void create()
{
PMQ.PMQConstants.DEBUG_LEVEL(5);
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
    if(!CLIENT->connect()) 
	return 0;
werror("connected.\n");
   return 1;
   }


  else return 1;
}

int listen(object id, string destination)
{
   if(READER) destruct(READER);
    READER = CLIENT->get_topic_reader(destination);
}

mixed parse(object id)
{
  gc();
   PMQ.Message.PMQMessage m;

werror("*** parse: %O\n\n", id->variables); 
   int timeout = id->variables->timeout;
   if(timeout) timeout/=1000;
   else timeout=30;

   if(CLIENT && id->variables->shutdown)
   {
     READER = 0;
     CLIENT->disconnect();
     CLIENT = 0;
     return "ok";
   }
   if(id->variables->message) id->misc->session_variables->message = id->variables->message;

   if(!CLIENT)
   {
     if(!connect(id))
     {
       CLIENT = 0;
       return "PMQD not available.";
     }
     sleep(2);
   }

   if(CLIENT && id->variables->type == "listen")     
   {                                       
     listen(id, id->variables->destination);
     return "ok";
   }  

  if(!READER) return "no reader specified";
  string messages = "";
/*
  do
  {
    m = READER->read(1);
    if(m)
    {
      messages += format_message(m);
    }
  }
  while(m);
*/
  if(!sizeof(messages))
  { 
    m = 0;
    if(READER && catch(m = READER->read(timeout)))
    {
      CLIENT = 0;
      READER = 0;
    }
    else if(m)
    {
      messages += format_message(m);
    }
  }
werror("messages: %O\n", messages);
  return Caudium.HTTP.string_answer("<?xml version=\"1.0\"?><ajax-response><messages id='" + (id->misc->session_variables->message||"id") + "'><message>" + messages + "</message></messages></ajax-response>", "text/xml");
}

string format_message(object m)
{
  string message = "<message";

  foreach(m->headers; string k; string v)
    message += (" " + lower_case(k) + "=\"" + v + "\"");
  message += ">";
  message += m->get_body();
  message += "</message>";
  return message;
}
void stop()
{
}
