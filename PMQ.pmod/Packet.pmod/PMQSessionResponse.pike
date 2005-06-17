  import PMQ; inherit .PMQPacket;
  string type = "SESSRESP";

  string message = "";

  void create()
  {
    msg_data->hostname = gethostname();
    msg_data->localtime = time();
  }

  int get_size()
  {
    return sizeof(encode_value(msg_data));
  }

  string get_data()
  {
    return encode_value(msg_data);
  }

  void set_session(string session)
  {
    this->msg_data->session = session;
  }

  string get_session()
  {
    return this->msg_data->session;
  }

  void set_code(int code)
  {
    this->msg_data->code = code;
  }

  int get_code()
  {
    return this->msg_data->code;
  }
