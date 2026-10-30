using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AwesomeAssertions;
using Microsoft.Extensions.Logging;
using Steeltoe.Informers.InformersBase.Tests.Utils;
using Xunit;

namespace Steeltoe.Informers.InformersBase.Tests
{
    public class ExtensionMethodsTests
    {

        [Fact]
        public void ToReset_WhenEmitEmpty_ProduceSingleItem()
        {

           var sut = new Dictionary<string,string>().ToReset(true).ToList();

           sut.Should().BeEquivalentTo(new[] { new ResourceEvent<string, string>(EventTypeFlags.ResetEmpty) });

        }
        [Fact]
        public void ToReset_TwoItems_CorrectReset()
        {
            var sut = new Dictionary<int,int>() {{1,1}, {2,2}}.ToReset(true).ToList();
            sut.Should().BeEquivalentTo(new[] { new ResourceEvent<int, int>(EventTypeFlags.ResetStart, 1, 1), new ResourceEvent<int, int>(EventTypeFlags.ResetEnd, 2, 2) });
        }
    }
}