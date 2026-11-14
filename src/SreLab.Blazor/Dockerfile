FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY ["SreLab.Blazor.csproj", "."]
RUN dotnet restore "SreLab.Blazor.csproj"
COPY . .
RUN dotnet publish "SreLab.Blazor.csproj" -c Release -o /app/publish
FROM nginx:alpine AS final
WORKDIR /usr/share/nginx/html
COPY --from=build /app/publish/wwwroot .
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 8080
CMD ["nginx", "-g", "daemon off;"]
