  import PMQ; 
  import PMQ.PMQConstants;
  inherit .PMQPacket;

  string type = "POSTMESSAGE";

  string encoded = "";
  Message.PMQMessage _message;
  string message_body;
  mapping headers;

  string _sprintf(mixed args)
  {
    return "PMQPostMessage(" + get_queue() + "," + get_message_id() + ")";
  }

string get_message_id()
{
  if(!_message) return "unknown";
  else return _message->get_header("pmq-message-id");
}

  void create()
  {
    msg_data->hostname = gethostname();
    msg_data->localtime = time();
  }

  int get_size()
  {
    msg_data->message_type = _message->_typeof();
    msg_data->message = (string)_message;
    encoded = encode_value(msg_data);
    return sizeof(encoded);
  }

  string get_data()
  {
    return encoded;
  }

  void set_ack(int flag)
  {
    this->msg_data->ack = flag;
  }

  int get_ack()
  {
    return this->msg_data->ack;
  }

  void set_pmqmessage(Message.PMQMessage m)
  {
    _message = m;
  }

  Message.PMQMessage get_pmqmessage()
  {
    return _message;
  }

  void set_queue(string queue)
  {
    this->msg_data->queue = queue;
  }

  string get_queue()
  {
    return this->msg_data->queue;
  }

  string get_session()
  {
    return this->msg_data->session;
  }

  void set_session(string session)
  {
    this->msg_data->session = session;
  }

  void parse(string payload)
  {
    ::parse(payload);
    DEBUG(2, "decoding message of type " + msg_data->message_type + "\n");
    program p = master()->resolv("PMQ.Message." + msg_data->message_type);
    
    if(!p)
      error("Unable to decode message of type " + msg_data->message_type + "\n");

    _message = p();
    _message->parse(MIME.Message(msg_data->message));
  }
  
