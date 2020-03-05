package org.marc.marcerators;

import org.apache.accumulo.core.iterators.IteratorEnvironment;

public class MockIteratorEnvironment implements IteratorEnvironment {

    @Override
    public boolean isSamplingEnabled() {
        return false;
    }
}
