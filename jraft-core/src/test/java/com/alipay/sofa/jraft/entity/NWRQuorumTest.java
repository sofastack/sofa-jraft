package com.alipay.sofa.jraft.entity;

import com.alipay.sofa.jraft.JRaftUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Akai
 */
public class NWRQuorumTest {
    private NWRQuorum nwrQuorum;
    private final Integer readFactor = 4;
    private final Integer writeFactor = 6;

    @Before
    public void setup() {
        this.nwrQuorum = new NWRQuorum(writeFactor, readFactor);
        this.nwrQuorum.init(JRaftUtils.getConfiguration("localhost:8081,localhost:8082,localhost:8083," +
                "localhost:8084,localhost:8085"), null);

    }

    @Test
    public void testGrant() {
        PeerId peer1 = new PeerId("localhost", 8081);
        this.nwrQuorum.grant(peer1);
        assertFalse(this.nwrQuorum.isGranted());

        PeerId peer2 = new PeerId("localhost", 8082);
        this.nwrQuorum.grant(peer2);
        assertFalse(this.nwrQuorum.isGranted());

        PeerId unfoundPeer = new PeerId("localhost", 8086);
        this.nwrQuorum.grant(unfoundPeer);
        assertFalse(this.nwrQuorum.isGranted());

        PeerId peer3 = new PeerId("localhost", 8083);
        this.nwrQuorum.grant(peer3);
        assertTrue(this.nwrQuorum.isGranted());
    }
}
