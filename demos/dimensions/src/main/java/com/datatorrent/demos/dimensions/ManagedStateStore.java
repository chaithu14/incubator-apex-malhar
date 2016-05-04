package com.datatorrent.demos.dimensions;

import java.util.ArrayList;
import java.util.List;

import org.apache.apex.malhar.lib.state.managed.ManagedTimeStateImpl;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.fileaccess.FileAccessFSImpl;
import com.datatorrent.lib.join.JoinStore;
import com.datatorrent.lib.join.TimeEvent;
import com.datatorrent.netlet.util.Slice;

public class ManagedStateStore extends ManagedTimeStateImpl implements JoinStore
{
  @Override
  public List<?> getValidTuples(Object tuple)
  {
    TimeEvent te = (TimeEvent)tuple;
    Object key = te.getEventKey();
    Slice value = super.getSync(0,new Slice(key.toString().getBytes()));
    List<Object> output = new ArrayList<>();
    if(value != null) {
      String ob = value.buffer.toString();
      output.add(ob);
    }
    return output;
  }

  @Override
  public boolean put(Object tuple)
  {
    TimeEvent te = (TimeEvent)tuple;
    Object key = te.getEventKey();
    Object value = te.getValue();
    super.put(0,te.getTime(),new Slice(key.toString().getBytes()), new Slice(value.toString().getBytes()));
    return true;
  }

  @Override
  public List<?> getUnMatchedTuples()
  {
    return null;
  }

  @Override
  public void isOuterJoin(boolean isOuter)
  {
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    ((FileAccessFSImpl)getFileAccess()).setBasePath(context.getValue(DAG.APPLICATION_PATH) + "/" + "bucket_data");
    super.setup(context);
  }
}
