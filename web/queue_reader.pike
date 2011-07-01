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
    CLIENT = PMQ.PMQClient("pmq://127.0.0.1:9998");
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
     return "<ajax-response>ok</ajax-response>";
   }
   if(id->variables->message) id->misc->session_variables->message = id->variables->message;

mixed err = catch {
   if(!CLIENT)
   {
     if(!connect(id))
     {
       CLIENT = 0;
       throw(Error.Generic("PMQD not available."));
     }
     sleep(2);
   }

   if(CLIENT && id->variables->type == "listen")     
   {                                       
     listen(id, id->variables->destination);
     return "<ajax-response>ok</ajax-response>";
   }  

  if(!READER) throw(Error.Generic("no reader specified"));
  string messages = "";

  while(READER->have_messages())
  {
werror("READING\n");    
m = READER->read(0.0);
werror("GOT: %O\n", m);
    if(m)
    {
      messages += format_message(m);
    }
  }

  if(!sizeof(messages))
  { 
    m = 0;
    mixed erro;

    if(READER)
    {
      erro = catch(m = READER->read(timeout));
      if(erro)
      {
        CLIENT = 0;
        READER = 0;
        throw(Error.Generic(erro[0]));
      }
   
      if(m)
      {
        messages += format_message(m);
      }
    }
  }
  werror("messages: %O\n", messages);
  return Caudium.HTTP.string_answer("<?xml version=\"1.0\"?><ajax-response><messages id='" + (id->misc->session_variables->message||"id") + "'>" + messages + "</messages></ajax-response>", "text/xml");
};
if(err)
{
report_error(describe_backtrace(err));
  return "<ajax-error>" + err[0] + "</ajax-error>";
}
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
