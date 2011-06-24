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
//werror("starting...\n");
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

int submit_ack()
{
  return (mode & MODE_SUBMIT_ACK);
}

int deliver_ack()
{
return 0;
  return (mode & MODE_DELIVER_ACK);
}

int send_message(Message.PMQMessage message)
{
  DEBUG(1, "%O->send_message(%O)\n", this, message);
  //werror( "%O->send_message(%O)\n", this, message);
  int r;
int a = deliver_ack();
mixed c = get_connection();
function d = c->send_message;
//werror("sending message from session %O\n", System.gettimeofday()[1] - get_connection()->st);
  r = d(message, this, a);

  if(started) call_out(get_message, 0);

  return r;
}

void acknowledge(string messageid)
{
  queue->acknowledge(messageid);
}

void unacknowledge(string messageid)
{
  queue->unacknowledge(messageid);
}

void get_message()
{
//werror("queue: %O\n", queue);
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
//werror("set_queue: %O\n", queue);
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
