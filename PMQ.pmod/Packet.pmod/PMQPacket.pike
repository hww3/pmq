  string type = "UNKNOWN";
  string message;
  mapping msg_data = ([]);

  mixed cast(string type)
  {
    if(type == "string")
    {
      return sprintf("PMQ%4c", sizeof(this->type) +1 + get_size()) + 
          sprintf("%c%s", sizeof(this->type), this->type) + get_data();
    }
    else error("unknown type %s", type);
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

    msg_data = decode_value(message);
  }
