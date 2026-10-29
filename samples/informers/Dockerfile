FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY ["global.json", "Directory.Build.props", "Directory.Packages.props", "./"]
COPY ["src/InformersBase/Steeltoe.Informers.InformersBase.csproj", "src/InformersBase/"]
COPY ["src/KubernetesBase/Steeltoe.Informers.KubernetesBase.csproj", "src/KubernetesBase/"]
COPY ["src/KubernetesCore/Steeltoe.Informers.KubernetesCore.csproj", "src/KubernetesCore/"]
COPY ["samples/informers/informers.csproj", "samples/informers/"]
RUN dotnet restore "samples/informers/informers.csproj"
COPY . .
RUN dotnet publish "samples/informers/informers.csproj" -c Release --no-restore -o /app

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS run
WORKDIR /app
COPY --from=build /app ./
ENTRYPOINT ["dotnet", "informers.dll"]
