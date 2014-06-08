#ifndef LOGGER_EXAMPLE_HPP_
#define LOGGER_EXAMPLE_HPP_

#include <EBSubscriber.hpp>
#include <SharedQueue.hpp>

class LoggerExample : public EBSubscriber {

public:

   LoggerExample();

   void sinkEvent(EBEvent event);

   bool hasEvents();

   void processEvents();

private:

   SharedQueue<EBEvent> rcvQueue;
};

#endif /* LOGGER_EXAMPLE_HPP_ */
