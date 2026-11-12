'use strict';

const { EventEmitter } = require('events');
const knx = require('knx');
const dptlib = require('knx/src/dptlib');
const PromiseQueue = require('promise-queue');

// The knx library swallows encoding errors in write() and then never calls the callback.
// Without a timeout, such a write occupies a queue slot forever; with 2 slots, two of
// those silently block every later write until the app restarts.
const WRITE_TIMEOUT_MS = 3000;

// When a tunneling request is not acknowledged, the knx library moves on without resetting its
// sequence counter. The KNX interface then discards every following request because the sequence
// number no longer matches, while receiving and heartbeats keep working. Per the KNXnet/IP spec
// the tunnel must be closed and reopened in that case, after which the lost telegram is resent.
const RESEND_MAX_AGE_MS = 15000;
const RECOVERY_TIMEOUT_MS = 20000;
const IN_FLIGHT_WINDOW_MS = 2500; // the library waits 2 s for an ACK

class KNXInterface extends EventEmitter {

  constructor(knxInterface) {
    super();

    // Setting object parameters from parameters from the interfacemanager
    this.name = knxInterface.interfaceName;
    this.ipAddress = knxInterface.interfaceIp;
    this.macAddress = knxInterface.interfaceMac;
    this.knxAddress = '15.15.255';

    // KNX interface settings/variabeles
    this.isConnected = false; // Boolean to keep track of the connection status.
    this.isTimedOut = false; // Boolean to keep track of timeout events.
    this.knxDisconnectCount = 0;
    this.isRecovering = false;
    this.lastWrites = new Map(); // groupaddress -> { value, datapoint, time }
    this.failedGroupAddresses = new Set();

    this.knxCommunicationQueue = new PromiseQueue(2, 100);

    // Learnmode variables
    this.learnMode = false;
    this.knxEventCaptures = [];

    // Setup logging and binding for fucntions
    this.log = console.log.bind(this, (`[Interface: ${this.name}]`));

    // Object arrays with callbacks
    this.onKNXConnectionCallbacks = [];
    this.onKNXEventCallbacks = {};

    this.log(`Creating interface with IP ${this.ipAddress}`);
    this.createConnection();
  }

  // Function to create the KNX connection
  createConnection() {
    // The KNX tunnel connection itself through the knx library
    if (this.isConnected === true) return; // Skip re-initializing when already connected.
    try {
      this.knxConnection = new knx.Connection({
        ipAddr: this.ipAddress, // IP address obtained through interfacemanager
        ipPort: 3671, // Fixed and part of the KNXnet/IP protocol.
        physAddr: this.knxAddress, // KNX address obtained through interfacemanager
        forceTunneling: true, // We want true tunnel connections
        loglevel: 'info',
        manualConnect: true, // The connection should be only opened if it's going to be used.
        minimumDelay: 10, // This sets the timeout between messages in ms
        // Setup the handlers that the library will use to respond on emits from FSM.js
        suppress_ack_ldatareq: false,
        handlers: {
          connected: () => {
            this.log('KNX Connected');
            // Change both connection booleans to their optimal state
            this.isConnected = true;
            this.isTimedOut = false;

            this.onKNXConnectionCallbacks.forEach((connCallback) => {
              try {
                connCallback('connected');
              } catch (error) {
                this.log(error);
              }
            });
          },
          disconnected: () => {
            this.log('KNX Disconnected');
            this.isConnected = false;

            this.onKNXConnectionCallbacks.forEach((callback) => {
              try {
                callback('disconnected');
              } catch (error) {
                this.log(error);
              }
            });
          },
          event: this.onKNXEventListener.bind(this),
          timeout: () => {
            this.isTimedOut = true;
            this.log('FSM timeout received');
          },
          error: (connstatus) => {
            this.log('Error from FSM:', connstatus);
          },
          tunnelreqfailed: (datagram) => this._onTunnelRequestFailed(datagram),
        },
      });
    } catch (error) {
      this.log('KNX lib error', error); // This should be able to catch the 'no valid ipv4 interfaces' error
    }
  }

  // ////////// KNX IP Interface and connection stuff ////////////

  // Function to (re)open the connection to the KNX network
  _connectKNX() {
    if (!this.knxConnection) return; // ensure to have a KNX connection instance.
    if (this.isConnected !== true) {
      this.log('Trying to (re)connect');
      try {
        this.knxConnection.Connect();
      } catch (error) {
        this.log('KNX lib connect error', error);
      }
    } else {
      this.log('Already connected');
    }
  }

  // Function to update the IP address if a change is detected by the interfacemanager.
  updateIP(newIPaddress) {
    if (newIPaddress === this.ipAddress) {
      this.log('IP has not changed, maintaining current connection');
    } else {
      this.log('Creating new KNX tunnel connection');
      this.ipAddress = newIPaddress;
      this._reconnect();
    }
  }

  // A telegram was not acknowledged by the KNX interface: reset the tunnel and resend it.
  _onTunnelRequestFailed(datagram) {
    const groupaddress = datagram && datagram.cemi && datagram.cemi.dest_addr;
    if (groupaddress) this.failedGroupAddresses.add(String(groupaddress));
    if (this.isRecovering) return;

    this.isRecovering = true;
    this.isConnected = false;
    const connection = this.knxConnection;
    const failedAt = Date.now();
    this.log(`No ACK for telegram to ${groupaddress}, resetting the KNX tunnel`);

    // machina (the library's state machine) has no once(); unsubscribe manually.
    const subscriptions = [];
    const once = (event, handler) => {
      const subscription = connection.on(event, (...args) => {
        subscription.off();
        handler(...args);
      });
      subscriptions.push(subscription);
    };
    const finish = () => {
      clearTimeout(watchdog);
      subscriptions.forEach((subscription) => subscription.off());
      this.isRecovering = false;
    };

    // Last resort if the reset does not complete: throw the connection away and build a new one.
    const watchdog = setTimeout(() => {
      this.log('Tunnel reset did not complete, rebuilding the KNX connection');
      finish();
      this._rebuildConnection();
      this.emit('tunnel_reset', groupaddress, []);
    }, RECOVERY_TIMEOUT_MS);

    once('connected', () => {
      if (connection !== this.knxConnection) return;
      finish();
      const resent = this._resendFailedWrites();
      this.emit('tunnel_reset', groupaddress, resent);
    });
    once('disconnected', () => {
      if (connection !== this.knxConnection) return;
      setTimeout(() => {
        try {
          connection.Connect();
        } catch (error) {
          this.log('Reconnect after tunnel reset failed', error);
        }
      }, 500);
    });

    // Only close the tunnel in a state without a pending UDP send: the library's send callback
    // transitions the state machine itself and would otherwise pull it out of 'disconnecting'.
    const closeTunnel = () => {
      if (connection !== this.knxConnection || !this.isRecovering) return;
      if (!['idle', 'sendTunnReq_waitACK', 'requestingConnState'].includes(connection.state)) {
        setTimeout(closeTunnel, 50);
        return;
      }
      // Telegrams queued inside the library carry the old tunnel's channel number and would be
      // rejected by the new tunnel, so drop that queue and resend everything written since the
      // unacknowledged telegram went out (it was sent up to IN_FLIGHT_WINDOW_MS before failedAt).
      // Relative dim steps (DPT3) are skipped because repeating them would dim twice.
      for (const [address, write] of this.lastWrites) {
        const isRelative = String(write.datapoint || '').toUpperCase().startsWith('DPT3');
        if (write.time >= failedAt - IN_FLIGHT_WINDOW_MS && !isRelative) this.failedGroupAddresses.add(address);
      }
      try {
        connection.clearQueue();
      } catch (error) {
        this.log('Could not clear the library queue', error);
      }
      // Stop the library's keepalive timer so it cannot fire in the middle of the reset.
      clearTimeout(connection.idletimer);
      connection.idletimer = null;
      try {
        connection.transition('disconnecting'); // sends DISCONNECT_REQUEST, then emits 'disconnected'
      } catch (error) {
        this.log('Could not close the tunnel cleanly', error);
        connection.emit('disconnected');
      }
    };
    closeTunnel();
  }

  // Replace the library connection with a brand-new one.
  _rebuildConnection() {
    const old = this.knxConnection;
    if (old) {
      clearInterval(old.connecttimer);
      clearTimeout(old.idletimer);
      old.idletimer = null;
      try {
        old.socket.close();
      } catch (error) {
        // socket already closed
      }
      try {
        old.off(); // drop all listeners so the old connection can no longer touch our state
      } catch (error) {
        // ignore
      }
    }
    this.isConnected = false;
    this.knxConnection = null;
    this.createConnection();
    this._connectKNX();
  }

  _waitForRecovery(maxWaitMs = 5000) {
    return new Promise((resolve) => {
      const started = Date.now();
      const check = () => {
        if (!this.isRecovering || Date.now() - started >= maxWaitMs) resolve();
        else setTimeout(check, 100);
      };
      check();
    });
  }

  _resendFailedWrites() {
    const now = Date.now();
    const resent = [];
    for (const groupaddress of this.failedGroupAddresses) {
      const last = this.lastWrites.get(groupaddress);
      if (last && now - last.time <= RESEND_MAX_AGE_MS) {
        resent.push(groupaddress);
        this.writeKNXGroupAddress(groupaddress, last.value, last.datapoint)
          .catch((error) => this.log(`Resend to ${groupaddress} failed`, error));
      }
    }
    this.failedGroupAddresses.clear();
    return resent;
  }

  // Safely disconnect and reconnect the KNX connection
  _reconnect() {
    if (!this.knxConnection) {
      this.isConnected = false;
      this.createConnection();
      return;
    }
    try {
      this.knxConnection.Disconnect(() => {
        this.isConnected = false;
        this.createConnection();
      });
    } catch (error) {
      this.log('Error during disconnect, forcing reconnect', error);
      this.isConnected = false;
      this.createConnection();
    }
  }

  // Function which returns the connected (or last connected) IP Address
  getConnectedIPAddress() {
    return this.ipAddress;
  }

  // KNX connection listener. Stores the given callback
  onKNXConnectionListener(callback) {
    if (this.isConnected) {
      callback('connected');
    }
    this.onKNXConnectionCallbacks.push(callback); // list with KNX callback to update on connecting.
    // console.log('KNXConnection callback list updated');
  }

  removeKNXConnectionListener(callback) {
    this.onKNXConnectionCallbacks = this.onKNXConnectionCallbacks.filter((cb) => cb !== callback);
    // console.log('KNXConnection callback list updated');
  }

  // KNX busevent listener, callbacks are added per groupaddress
  addKNXEventListener(groupaddress, callback) {
    if (typeof groupaddress === 'string' && groupaddress !== '') {
      if (!this.onKNXEventCallbacks[groupaddress]) {
        this.onKNXEventCallbacks[groupaddress] = [callback];
      } else {
        this.onKNXEventCallbacks[groupaddress].push(callback);
      }
    }
  }

  // Removes all listeners attached the the provided callback
  removeKNXEventListener(groupaddress, callback) {
    if (typeof groupaddress === 'string' && groupaddress !== '') {
      if (this.onKNXEventCallbacks[groupaddress]) {
        this.onKNXEventCallbacks[groupaddress] = this.onKNXEventCallbacks[groupaddress]
          .filter((cb) => cb !== callback);
      }
    }
  }

  // KNX busevent listener, triggered when an KNX event occurs
  onKNXEventListener(event, source, destination, value) {
    // Emit the event, and if there is a callback for the destiniation addres call it.
    if (!destination) return;
    this.emit('knx_event', event, destination, value);
    if (this.onKNXEventCallbacks[destination] && (event === 'GroupValue_Write' || event === 'GroupValue_Response')) {
      this.onKNXEventCallbacks[destination].forEach((callback) => {
        if (!callback || typeof callback !== 'function') return;
        callback(destination, value);
      });
    }
    // If the learnmode is turned on, also store the event in de capture array.
    if (this.learnMode) {
      this.log('Pushing event to learn mode store');
      this.knxEventCaptures.push({
        event,
        destination,
        source,
        value,
      });
    }
  }

  // ////////// KNX Group Address Stuff ////////////

  // Write a groupadress with the given value and datapoint type
  async writeKNXGroupAddress(groupaddress, value, datapoint) {
    // Check if the groupadress is not empty
    if (groupaddress && groupaddress !== '') {
      // During a tunnel reset (about a second), wait for the new tunnel instead of failing.
      if (this.isRecovering) await this._waitForRecovery();
      if (this.isConnected === true && !this.isTimedOut) {
        // Validate before queueing: the knx lib swallows these errors and never calls back.
        if (value === undefined || value === null) {
          throw new Error(`knx_invalid_value: ${value} for ${groupaddress}`);
        }
        try {
          dptlib.populateAPDU(value, {}, datapoint);
        } catch (error) {
          this.log(`Rejected write of ${value} (${datapoint || 'DPT1'}) to ${groupaddress}:`, error);
          throw new Error(`knx_invalid_value: ${error}`);
        }

        // Only add the request in the queue when the connection is openend.
        this.log(`Writing ${value} to ${groupaddress}`);
        this.lastWrites.set(String(groupaddress), { value, datapoint, time: Date.now() });
        return this.knxCommunicationQueue.add(() => new Promise((resolve, reject) => {
          const timer = setTimeout(() => {
            this.log(`Write to ${groupaddress} never completed, releasing queue slot`);
            reject(new Error(`knx_write_timeout: ${groupaddress}`));
          }, WRITE_TIMEOUT_MS);
          try {
            this.knxConnection.write(groupaddress, value, datapoint, (error) => {
              clearTimeout(timer);
              if (error) reject(error);
              else resolve();
            });
          } catch (error) {
            clearTimeout(timer);
            reject(error);
          }
        }));
      }
      ++this.knxDisconnectCount;
      throw new Error('knx_no_connection');
    }
    throw new Error('knx_no_groupaddress');
  }

  // Function to read the value for the given groupadress
  async readKNXGroupAddress(groupaddress) {
    if (groupaddress && groupaddress !== '') {
      // Check if the groupadress is not empty
      if (this.isConnected === true && !this.isTimedOut) {
        return this.knxCommunicationQueue.add(async () => {
          // Only add the request in the queue when the connection is openend.
          return new Promise((resolve, reject) => {
            this.knxConnection.read(groupaddress, (src, data) => resolve(data));
            // Trigger a timeout, inherited from the library timeout (3s)
            setTimeout(() => reject(new Error(`knx_read_timeout: ${groupaddress}`)), 3000);
          });
        });
      }
      ++this.knxDisconnectCount;
      throw new Error('knx_no_connection');
    }
    throw new Error('knx_no_groupaddress');
  }

  // Run the learnmode for the given time.
  async learnModeSwitch(time) {
    this.knxEventCaptures = [];
    if (this.learnMode === true) throw new Error('already_learmode');
    this.learnMode = true; // start the learnmode

    await this._wait(time);
    this.learnMode = false;
    return this.filterKNXEvents(this.knxEventCaptures);
  }

  /*
    Async wait function
   */
  _wait(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /* Function to filter the captured events from the learnmode.
  Duplicate entries will be reduced to one entry, which will be counted.
  The list will then be sorted by event occurrence if possible.
  */
  filterKNXEvents(list) {
    const filterSet = new Set();
    const countObj = {};
    for (const obj of list) {
      if (filterSet.has(obj.destination)) {
        countObj[obj.destination]++;
      } else {
        countObj[obj.destination] = 1;
        filterSet.add(obj.destination);
      }
    }
    list = list.sort((a, b) => {
      return (countObj[b.destination] - countObj[a.destination]);
    });
    let filteredList = [];
    list.forEach((event) => {
      if (event.value.readUInt8() === 0 || event.value.readUInt8() === 1) {
        filteredList.push(event);
      }
    });
    filteredList = filteredList.filter((event, pos, arr) => {
      return arr.map((mapObj) => mapObj['destination'])
        .indexOf(event['destination']) === pos;
    });
    return filteredList;
  }

}

module.exports = KNXInterface;
