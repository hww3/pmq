  import PMQ; inherit .PMQPacket;
  string type = "ACK";

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

  void set_msg_id(string msg_id)
  {
    this->msg_data->msg_id = msg_id;
  }

  int get_msg_id()
  {
    return this->msg_data->msg_id;
  }

  int get_code()
  {
    return this->msg_data->code;
  }

  void set_code(int code)
  {
    this->msg_data->code = code;
  }

  void set_session(string session)
  {
    this->msg_data->session = session;
  }

  string get_session()
  {
    return this->msg_data->session;
  }
