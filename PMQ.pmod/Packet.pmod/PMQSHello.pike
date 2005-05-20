  inherit .PMQPacket;
  string type = "SHELLO";

  string message = "";

  void create()
  {
    msg_data->hostname = gethostname();
    msg_data->localtime = time();

  }

  int get_size()
  {
    message = encode_value(msg_data);
    return sizeof(message);
  }

  string get_data()
  {
    message = encode_value(msg_data);
    return message;
  }

  array get_versions()
  {
    return msg_data->protocol_versions;
  }
 
  void set_versions(array v)
  {
    msg_data->protocol_versions = v;
  }
 
  void parse(string payload)
  {
    msg_data = decode_value(payload);
  }
