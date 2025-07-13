FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/aspnet:9.0-alpine AS base
USER $APP_UID
WORKDIR /app
EXPOSE 8080

FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:9.0-alpine AS build
ARG TARGETARCH
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["Rinha/Rinha.csproj", "Rinha/"]
RUN dotnet restore "Rinha/Rinha.csproj" --arch $TARGETARCH
COPY . .
WORKDIR "/src/Rinha"
RUN dotnet build "./Rinha.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./Rinha.csproj" -c $BUILD_CONFIGURATION --arch $TARGETARCH -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Rinha.dll"]
