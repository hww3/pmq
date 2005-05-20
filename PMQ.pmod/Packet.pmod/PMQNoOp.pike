  inherit .PMQPacket;
  string type = "NOOP";

  string message = "";

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

  string parse(string payload)
  {
    message = payload;
  }
