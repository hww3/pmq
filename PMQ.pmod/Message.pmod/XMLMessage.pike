import PMQ;
inherit .PMQMessage;

string _type = "XMLMessage";

Public.Parser.XML2.Node node;


void set_node(Public.Parser.XML2.Node node)
{
  this->node = node;
}

Public.Parser.XML2.Node get_node()
{
  return this->node;
}

mixed cast(string type)
{
  message = Public.Parser.XML2.render_xml(this->node);
  return ::cast(type);
}

void parse(MIME.Message payload)
{
  ::parse(payload);
  this->node = Public.Parser.XML2.parse_xml(get_body());
}
