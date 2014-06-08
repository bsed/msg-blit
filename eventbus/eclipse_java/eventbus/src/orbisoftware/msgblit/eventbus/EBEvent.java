/*
 *  msg-blit - Implementation of various messaging patterns.
 *  Event Bus - Simple implementation of an event bus utilizing 
 *  observer pattern.
 *  
 *  Copyright (C) 2014 Harlan Murphy
 *  orbisoftware@gmail.com
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License Version 3 dated 29 June 2007, as published by the
 *  Free Software Foundation.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with restful-dds; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */

package orbisoftware.msgblit.eventbus;

public class EBEvent {

   public int publisherID;    // Unique DDS publisher identifier
   public int eventID;        // Unique event identifier
   public short eventCatType; // Event category type (see category enumerations)
   public short eventDefType; // Event definition type (see event enumerations)
   public String eventData;   // Event data
}
