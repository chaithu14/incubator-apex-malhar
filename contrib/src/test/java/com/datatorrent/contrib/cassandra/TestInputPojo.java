/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.cassandra;


/*
 * TestInputPojo class specified by user for specifying schema as part of testing.
 * CassandraPOJOInputOperator can be used to get Field values from cassandra database columns
 * and set in this pojo as an example.
 */
public class TestInputPojo
{
  private int id;

  public int getId()
  {
    return id;
  }

  public void setId(int id)
  {
    this.id = id;
  }
  private String lastname;
  private int age;


  public int getAge()
  {
    return age;
  }

  public void setAge(int age)
  {
    this.age = age;
  }

  public String getLastname()
  {
    return lastname;
  }

  public void setLastname(String lastname)
  {
    this.lastname = lastname;
  }

}