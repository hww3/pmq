import PMQ;
string session_id;
int mode;
import PMQConstants;
PMQSConnection conn;
Queue.PMQQueue queue;
int started = 0;

void create()
{

}

string _sprintf(mixed ... args)
{
  return "PMQSSession(" + get_session_id() + ")";
}

void start()
{
  started = 1;
werror("starting...\n");
  if(started) call_out(get_message, 0);
/*
  if(queue)
    queue->process_queue();
*/
}

void stop()
{
  started = 0;
  if(queue)
    queue->unget_message(this);  
}

int send_message(Message.PMQMessage message, int ack)
{
  DEBUG(1, "%O->send_message(%O, %O)\n", this, message, ack);
  int r;
  r = get_connection()->send_message(message, this, ack);

  if(started) call_out(get_message, 0);

  return r;
}

void get_message()
{
  queue->get_message(this);  
}

void set_session_id(string session_id)
{
  this->session_id = session_id;
}

void set_mode(int mode)
{
  this->mode = mode;
}

int get_mode()
{
  return this->mode;
}

void set_connection(PMQSConnection conn)
{
  this->conn = conn;
}

string get_session_id()
{
  return this->session_id;
}

PMQSConnection get_connection()
{
  return this->conn;
}
Queue.PMQQueue get_queue()
{
  return this->queue;
}

void set_queue(Queue.PMQQueue queue)
{
  this->queue = queue;
}

void destroy()
{
  stop();
  if(queue)
  {
    queue->unsubscribe(this);
  }
}
