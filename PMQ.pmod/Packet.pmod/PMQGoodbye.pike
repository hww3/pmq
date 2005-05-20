  inherit .PMQPacket;
  string type = "GOODBYE";

  string message = "it was nice chatting.\n";

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
