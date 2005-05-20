constant MODE_LISTEN = 2;
constant MODE_WRITE = 3;

constant CODE_SUCCESS = 1;
constant CODE_FAILURE = 0;

constant MODE_BLOCK = 250;
constant MODE_NONBLOCK = 251;

int debug_level = 10;

void DEBUG_LEVEL(int level)
{
  debug_level = level;
}

void DEBUG(int level, mixed ... args)
{
  if(level <= debug_level)
    werror(@args);
}
