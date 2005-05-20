string session_id;
int mode;
PMQSConnection conn;
Queue.PMQQueue queue;

void create()
{

}

string _sprintf(mixed ... args)
{
  return "PMQSSession(" + get_session_id() + ")";
}

int send_message(Message.PMQMessage message)
{
  return get_connection()->send_message(message, this);
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



