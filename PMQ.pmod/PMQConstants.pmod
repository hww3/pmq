constant MODE_LISTEN = 2;
constant MODE_WRITE = 1;

constant MODE_DELIVER_ACK = 4;
constant MODE_SUBMIT_ACK = 8;
constant MODE_PERSIST = 8;

//!
constant CODE_SUCCESS = 1;

//!
constant CODE_FAILURE = 0;

//!
constant CODE_NOACCESS = 4;

//!
constant CODE_NOTFOUND = 2;

//!
constant CODE_NOSLOTS = 5;

//!
constant TYPE_PTP = 1;

//!
constant TYPE_TOPIC = 2;

int debug_level = 10;

//!
void DEBUG_LEVEL(int level)
{
  debug_level = level;
}

void DEBUG(int level, mixed ... args)
{
//return;
  if(level <= debug_level)
    werror(@args);
}
