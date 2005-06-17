  import PMQ;

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


  void set_reply_id(int reply_id)
  {
    this->msg_data->reply_id = reply_id;
  }

  int get_reply_id()
  {
    return this->msg_data->reply_id;
  }


  void set_id(int id)
  {
    this->msg_data->id = id;
  }

  int get_id()
  {
    return this->msg_data->id;
  }



