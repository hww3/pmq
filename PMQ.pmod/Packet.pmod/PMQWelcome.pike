  import PMQ; inherit .PMQPacket;
  string type = "WELCOME";

  string message = "";

  void create()
  {
  }

  int get_size()
  {
    return sizeof(encode_value(msg_data));
  }

  string get_data()
  {
    return encode_value(msg_data);
  }

  void set_client_id(string client_id)
  {
    msg_data->client_id = client_id;
  }

  string get_client_id()
  {
    return msg_data->client_id;
  }

  string parse(string payload)
  {
    msg_data = decode_value(payload);
  }
