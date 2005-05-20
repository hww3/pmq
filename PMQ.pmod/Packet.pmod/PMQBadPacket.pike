  inherit .PMQPacket;
  string type = "BADPACKET";

  string message = "bad packet.\n";

  void create()
  {
  }

  int get_size()
  {
    return sizeof(message);
  }

  string get_data()
  {
    return message;
  }

  void parse(string payload)
  {
    message = payload;
  }
