package cmd

import (
	"fmt"
	"os"

	"github.com/equinor/seismic-cloud/api/config"
	"github.com/equinor/seismic-cloud/api/server"
	"github.com/equinor/seismic-cloud/api/service"
	"github.com/spf13/cobra"
	jww "github.com/spf13/jwalterweatherman"
	"github.com/spf13/viper"
)

// serveCmd represents the serve command
var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "serve seismic cloud provider",
	Long:  `serve seismic cloud provider.`,
	Run:   runServe,
}

func runServe(cmd *cobra.Command, args []string) {

	if viper.ConfigFileUsed() == "" {
		jww.ERROR.Println("No config file loaded")
		os.Exit(1)
	} else {
		jww.INFO.Println("Using config file:", viper.ConfigFileUsed())
	}

	opts := make([]server.HttpServerOption, 0)

	if config.UseAuth() {
		opts = append(opts,
			server.WithOAuth2(config.AuthServer(), "seismic-api"))
	}

	opts = append(
		opts,
		server.WithManifestStore(&service.ManifestFileStore{BasePath: "cubes"}))

	opts = append(
		opts,
		server.WithStitchCommand([]string{"ls"}))

	hs, err := server.NewHttpServer(opts...)

	if err != nil {
		fmt.Println("Error starting http server: ", err)
		os.Exit(1)
	}
	hs.Serve()
}

func init() {
	rootCmd.AddCommand(serveCmd)
}
