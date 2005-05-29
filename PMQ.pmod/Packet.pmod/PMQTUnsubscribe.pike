  import PMQ; inherit .PMQPacket;
  string type = "TUNSUBSCRIBE";

  string message = "";
  string encoded = "";

  void create()
  {
    msg_data->hostname = gethostname();
    msg_data->localtime = time();
  }

  int get_size()
  {
    encoded = encode_value(msg_data);
    return sizeof(encoded);

  }

  string get_data()
  {
    return encoded;
  }

  string get_topic()
  {
    return msg_data->topic;
  }

  void set_topic(string topic)
  {
    this->msg_data->topic = topic;
  }

  void set_session(string session)
  {
    this->msg_data->session = session;
  }

  string get_session()
  {
    return this->msg_data->session;
  }

  void set_mode(int mode)
  {
    this->msg_data->mode = mode;
  }

  int get_mode()
  {
    return this->msg_data->mode;
  }

