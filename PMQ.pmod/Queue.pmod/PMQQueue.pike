import PMQ;
string name;

import PMQConstants;

int processing = 0;
multiset listeners = (<>);
multiset writers = (<>);

void create(string name)
{
  this->name = name;
}

void trigger_process_queue()
{
}

void start()
{
}

void stop()
{

}

int post_message(Message.PMQMessage message, PMQSSession session);
int subscribe(PMQSSession listener);
int unsubscribe(PMQSSession listener);

class MessageWrapper
{
  
}
