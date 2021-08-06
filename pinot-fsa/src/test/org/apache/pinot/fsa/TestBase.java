package org.apache.pinot.fsa;

import org.junit.runner.RunWith;

import com.carrotsearch.randomizedtesting.MixWithSuiteName;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.SeedDecorators;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction.Action;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakGroup;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakGroup.Group;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies.Consequence;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

@RunWith(RandomizedRunner.class)
@SeedDecorators({MixWithSuiteName.class})
@ThreadLeakScope(Scope.SUITE)
@ThreadLeakGroup(Group.MAIN)
@ThreadLeakAction({Action.WARN, Action.INTERRUPT})
@ThreadLeakLingering(linger = /* millis */ 5 * 1000)
@ThreadLeakZombies(Consequence.IGNORE_REMAINING_TESTS)
@TimeoutSuite(millis = /* millis */ 360 * 1000)
@ThreadLeakFilters(defaultFilters = true, filters = {TestBase.CustomThreadFilter.class})
public abstract class TestBase {
  /**
   * Any custom thread filters we should ignore.
   */
  public static class CustomThreadFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
      // IBM J9 bogus threads.
      String threadName = t.getName();
      if ("Attach API wait loop".equals(threadName) ||
          "file lock watchdog".equals(threadName)   ||
          "ClassCache Reaper".equals(threadName)) {
        return true;
      }

      return false;
    }
  }
}
