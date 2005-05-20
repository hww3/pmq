  inherit .PMQPacket;
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

  void set_id(string id)
  {
    this->msg_data->id = id;
  }

  string get_id()
  {
    return this->msg_data->id;
  }

