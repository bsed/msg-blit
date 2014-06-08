#ifndef EVENT_EMITTER_EXAMPLE_HPP_
#define EVENT_EMITTER_EXAMPLE_HPP_

#include <EBRouter.hpp>

class EventEmitterExample {

public:

   EventEmitterExample(EBRouter* router, int emitterID);
   void start();

private:

   void run();

   EBRouter* router;
   int emitterID;
};

#endif /* EVENT_EMITTER_EXAMPLE_HPP_ */
