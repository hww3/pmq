  import PMQ; inherit .PMQPacket;
  string type = "NULL";

  string message = "";

  mixed cast(string type)
  {
    if(type =="string")
      return "";
  }

  void create()
  {
  }

  int get_size()
  {
    return 0;
  }

  string get_data()
  {
    return "";
  }

  string parse(string payload)
  {
    message = payload;
  }
