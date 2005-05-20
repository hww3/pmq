  inherit .PMQPacket;
  string type = "CHELLO";

  string encoded = "";

  array supported_versions = ({"1.0"});

  string message = "";

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

  void set_version(string v)
  {
    msg_data->protocol_version = v;
  }

  string get_client_id()
  {
    return msg_data->client_id;
  }

  string set_client_id(string client_id)
  {
    msg_data->client_id = client_id;
  }

  string get_version()
  {
    return msg_data->protocol_version;
  }

