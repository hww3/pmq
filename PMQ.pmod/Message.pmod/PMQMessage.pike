import PMQ;
string _type = "PMQMessage";

string message;
mapping headers = ([]);

void create()
{

}

string _sprintf(mixed ... args)
{
  return "PMQ.Message.PMQMessage(" + get_header("pmq-message-id") + ")";
}

string _typeof()
{
  return _type;
}

void set_header(string header, string value)
{
  if(headers[header]) headers[header] = value;
  else headers[header] = value;
}

string get_header(string header)
{
  return headers[lower_case(header)];
}

void del_header(string header)
{
  if(headers[header]) m_delete(headers, header);
}

void parse(MIME.Message m)
{
  message = m->getdata();
  headers = copy_value(m->headers);
}

string get_body()
{
  return message;
}

void set_body(string s)
{
  message = s;
}

string cast(string type)
{
  MIME.Message m = MIME.Message(message, headers);
  return (string)m;
}
