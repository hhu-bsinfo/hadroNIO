package de.hhu.bsinfo.hadronio.infinileap;

import de.hhu.bsinfo.hadronio.binding.UcxConnectionRequest;
import de.hhu.bsinfo.infinileap.binding.ConnectionRequest;

record InfinileapConnectionRequest(ConnectionRequest connectionRequest) implements UcxConnectionRequest {}
